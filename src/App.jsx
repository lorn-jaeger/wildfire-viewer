import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import mapboxgl from 'mapbox-gl'
import 'mapbox-gl/dist/mapbox-gl.css'
import './App.css'

const DEFAULT_LOOKBACK_DAYS = 7
const DEFAULT_MIN_AREA_KM2 = 1
const DEFAULT_ACTIVE_LIMIT = 500
const DEFAULT_AS_OF_DAY = '2021-08-19'
const EMPTY_GEOJSON = { type: 'FeatureCollection', features: [] }
const PLAYBACK_MS = 800
const COLOR_STEPS = ['#fde68a', '#fdba74', '#fb923c', '#f97316', '#ea580c', '#c2410c', '#7c2d12']

function getTodayUtcDay() {
  return new Date().toISOString().slice(0, 10)
}

function formatUtcDateTime(value) {
  if (!value) return 'n/a'
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return 'n/a'
  return parsed.toISOString().replace('T', ' ').slice(0, 16) + ' UTC'
}

function formatNumber(value, fractionDigits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return '0'
  }
  return Number(value).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: fractionDigits,
  })
}

function parseApiError(status, payload) {
  if (payload && typeof payload.error === 'string') {
    return payload.error
  }
  return `Request failed with status ${status}`
}

function extendBoundsWithGeometry(bounds, geometry) {
  if (!geometry || !geometry.type) return

  if (geometry.type === 'Point') {
    bounds.extend(geometry.coordinates)
    return
  }

  if (geometry.type === 'LineString') {
    for (const coord of geometry.coordinates) bounds.extend(coord)
    return
  }

  if (geometry.type === 'Polygon') {
    for (const ring of geometry.coordinates) {
      for (const coord of ring) bounds.extend(coord)
    }
    return
  }

  if (geometry.type === 'MultiPoint') {
    for (const coord of geometry.coordinates) bounds.extend(coord)
    return
  }

  if (geometry.type === 'MultiLineString') {
    for (const line of geometry.coordinates) {
      for (const coord of line) bounds.extend(coord)
    }
    return
  }

  if (geometry.type === 'MultiPolygon') {
    for (const polygon of geometry.coordinates) {
      for (const ring of polygon) {
        for (const coord of ring) bounds.extend(coord)
      }
    }
  }
}

function getFeatureStep(feature) {
  return Number(feature?.properties?.stepIndex || 0)
}

function filterToStep(features, maxStep) {
  return (features || []).filter((feature) => getFeatureStep(feature) <= maxStep)
}

function latestFeatureAtStep(features, maxStep) {
  const visible = filterToStep(features, maxStep)
  if (visible.length === 0) return null
  return visible.reduce((best, current) => (getFeatureStep(current) > getFeatureStep(best) ? current : best))
}

function AreaSparkline({ series }) {
  const points = useMemo(() => {
    if (!Array.isArray(series) || series.length === 0) return ''
    const width = 300
    const height = 80
    const padding = 8
    const values = series.map((item) => Number(item.areaCumulativeKm2) || 0)
    const min = Math.min(...values)
    const max = Math.max(...values)
    const range = Math.max(max - min, 0.0001)

    return values
      .map((value, index) => {
        const x = padding + (index / Math.max(values.length - 1, 1)) * (width - padding * 2)
        const y = height - padding - ((value - min) / range) * (height - padding * 2)
        return `${index === 0 ? 'M' : 'L'}${x.toFixed(2)} ${y.toFixed(2)}`
      })
      .join(' ')
  }, [series])

  if (!points) {
    return <p className="muted">No cumulative timeline in this window.</p>
  }

  return (
    <svg className="sparkline" viewBox="0 0 300 80" aria-label="Cumulative fire area timeline">
      <path d={points} />
    </svg>
  )
}

function App() {
  const mapContainerRef = useRef(null)
  const mapRef = useRef(null)
  const popupRef = useRef(null)
  const playIntervalRef = useRef(null)

  const [asOfDay, setAsOfDay] = useState(DEFAULT_AS_OF_DAY || getTodayUtcDay())
  const [lookbackDays, setLookbackDays] = useState(DEFAULT_LOOKBACK_DAYS)
  const [minAreaKm2, setMinAreaKm2] = useState(DEFAULT_MIN_AREA_KM2)

  const [mapReady, setMapReady] = useState(false)
  const [loadingActive, setLoadingActive] = useState(false)
  const [loadingDetail, setLoadingDetail] = useState(false)
  const [activeCount, setActiveCount] = useState(0)
  const [errorMessage, setErrorMessage] = useState('')
  const [lastActiveRequest, setLastActiveRequest] = useState('')
  const [lastDetailRequest, setLastDetailRequest] = useState('')

  const [selectedFireId, setSelectedFireId] = useState(null)
  const [selectedFireDetail, setSelectedFireDetail] = useState(null)
  const [visibleStep, setVisibleStep] = useState(0)
  const [isPlaying, setIsPlaying] = useState(false)

  const mapToken = import.meta.env.VITE_MAPBOX_TOKEN || ''
  const apiBaseUrl = (import.meta.env.VITE_API_BASE_URL || 'http://localhost:4000').replace(/\/$/, '')
  const missingToken = !mapToken

  const stepCount = selectedFireDetail?.series?.length || 0

  const stopPlayback = useCallback(() => {
    setIsPlaying(false)
    if (playIntervalRef.current) {
      window.clearInterval(playIntervalRef.current)
      playIntervalRef.current = null
    }
  }, [])

  const clearSelectedPerimeters = useCallback(() => {
    const map = mapRef.current
    if (!map) return

    const cumulativeSource = map.getSource('selected-cumulative')
    if (cumulativeSource) cumulativeSource.setData(EMPTY_GEOJSON)

    const growthSource = map.getSource('selected-growth')
    if (growthSource) growthSource.setData(EMPTY_GEOJSON)

    const latestSource = map.getSource('selected-latest')
    if (latestSource) latestSource.setData(EMPTY_GEOJSON)
  }, [])

  const applyVisiblePerimeters = useCallback((maxStep) => {
    const map = mapRef.current
    if (!map || !selectedFireDetail) return

    const cumulativeFeatures = selectedFireDetail?.perimeters_cumulative?.features || []
    const growthFeatures = selectedFireDetail?.perimeters_growth?.features || []

    const visibleCumulative = filterToStep(cumulativeFeatures, maxStep)
    const visibleGrowth = filterToStep(growthFeatures, maxStep)
    const latest = latestFeatureAtStep(cumulativeFeatures, maxStep)

    const cumulativeSource = map.getSource('selected-cumulative')
    if (cumulativeSource) {
      cumulativeSource.setData({ type: 'FeatureCollection', features: visibleCumulative })
    }

    const growthSource = map.getSource('selected-growth')
    if (growthSource) {
      growthSource.setData({ type: 'FeatureCollection', features: visibleGrowth })
    }

    const latestSource = map.getSource('selected-latest')
    if (latestSource) {
      latestSource.setData({
        type: 'FeatureCollection',
        features: latest ? [latest] : [],
      })
    }
  }, [selectedFireDetail])

  const loadActiveFires = useCallback(async () => {
    const map = mapRef.current
    if (!map || !asOfDay) return

    const safeLookback = Math.max(1, Math.min(Number(lookbackDays) || DEFAULT_LOOKBACK_DAYS, 30))
    const safeMinArea = Math.max(0, Number(minAreaKm2) || DEFAULT_MIN_AREA_KM2)

    const params = new URLSearchParams({
      asOf: asOfDay,
      lookbackDays: String(safeLookback),
      minAreaKm2: String(safeMinArea),
      limit: String(DEFAULT_ACTIVE_LIMIT),
      _t: String(Date.now()),
    })

    const requestUrl = `${apiBaseUrl}/active-fires?${params.toString()}`
    setLastActiveRequest(requestUrl)
    setLoadingActive(true)
    setErrorMessage('')

    try {
      const response = await fetch(requestUrl, { cache: 'no-store' })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(parseApiError(response.status, payload))
      }

      const featureCollection = {
        type: 'FeatureCollection',
        features: Array.isArray(payload.features) ? payload.features : [],
      }

      const source = map.getSource('active-fires')
      if (!source) {
        throw new Error('Map source "active-fires" is not ready yet.')
      }

      source.setData(featureCollection)
      setActiveCount(Number(payload.count ?? featureCollection.features.length))

      const visibleFireIds = new Set(
        featureCollection.features
          .map((feature) => Number(feature?.properties?.fireId))
          .filter((value) => Number.isFinite(value)),
      )

      if (selectedFireId && !visibleFireIds.has(selectedFireId)) {
        setSelectedFireId(null)
        setSelectedFireDetail(null)
        setVisibleStep(0)
        stopPlayback()
        clearSelectedPerimeters()
      }

      if (featureCollection.features.length > 0) {
        const bounds = new mapboxgl.LngLatBounds()
        for (const feature of featureCollection.features) {
          if (feature.geometry?.type === 'Point') {
            bounds.extend(feature.geometry.coordinates)
          }
        }
        if (!bounds.isEmpty()) {
          map.fitBounds(bounds, { padding: 56, duration: 700, maxZoom: 8.5 })
        }
      }
    } catch (error) {
      setActiveCount(0)
      setSelectedFireId(null)
      setSelectedFireDetail(null)
      setVisibleStep(0)
      stopPlayback()
      clearSelectedPerimeters()
      setErrorMessage(error instanceof Error ? error.message : 'Unable to load active fires')
    } finally {
      setLoadingActive(false)
    }
  }, [apiBaseUrl, asOfDay, clearSelectedPerimeters, lookbackDays, minAreaKm2, selectedFireId, stopPlayback])

  const loadSelectedFireDetail = useCallback(async () => {
    const map = mapRef.current
    if (!map || !selectedFireId) {
      clearSelectedPerimeters()
      return
    }

    const safeLookback = Math.max(1, Math.min(Number(lookbackDays) || DEFAULT_LOOKBACK_DAYS, 30))
    const params = new URLSearchParams({
      asOf: asOfDay,
      lookbackDays: String(safeLookback),
      mode: 'overpass',
      geometryMode: 'display',
      _t: String(Date.now()),
    })

    const requestUrl = `${apiBaseUrl}/active-fires/${selectedFireId}?${params.toString()}`
    setLastDetailRequest(requestUrl)
    setLoadingDetail(true)
    setErrorMessage('')

    try {
      const response = await fetch(requestUrl, { cache: 'no-store' })
      const payload = await response.json()
      if (!response.ok) {
        throw new Error(parseApiError(response.status, payload))
      }

      setSelectedFireDetail(payload)
      const maxStep = payload?.series?.length ? Number(payload.series[payload.series.length - 1].stepIndex) : 0
      setVisibleStep(maxStep)
      stopPlayback()

      const allCumulative = payload?.perimeters_cumulative?.features || []
      if (allCumulative.length > 0) {
        const bounds = new mapboxgl.LngLatBounds()
        for (const feature of allCumulative) {
          extendBoundsWithGeometry(bounds, feature.geometry)
        }
        if (!bounds.isEmpty()) {
          map.fitBounds(bounds, { padding: 72, duration: 700, maxZoom: 10 })
        }
      }
    } catch (error) {
      setSelectedFireDetail(null)
      setVisibleStep(0)
      stopPlayback()
      clearSelectedPerimeters()
      setErrorMessage(error instanceof Error ? error.message : 'Unable to load fire detail')
    } finally {
      setLoadingDetail(false)
    }
  }, [apiBaseUrl, asOfDay, clearSelectedPerimeters, lookbackDays, selectedFireId, stopPlayback])

  useEffect(() => {
    if (!selectedFireDetail || visibleStep <= 0) {
      clearSelectedPerimeters()
      return
    }
    applyVisiblePerimeters(visibleStep)
  }, [applyVisiblePerimeters, clearSelectedPerimeters, selectedFireDetail, visibleStep])

  useEffect(() => {
    if (stepCount <= 0) {
      stopPlayback()
      return
    }
    if (!isPlaying) return

    playIntervalRef.current = window.setInterval(() => {
      setVisibleStep((current) => {
        if (current >= stepCount) {
          stopPlayback()
          return stepCount
        }
        return current + 1
      })
    }, PLAYBACK_MS)

    return () => {
      if (playIntervalRef.current) {
        window.clearInterval(playIntervalRef.current)
        playIntervalRef.current = null
      }
    }
  }, [isPlaying, stepCount, stopPlayback])

  useEffect(() => {
    if (missingToken || !mapContainerRef.current) {
      return
    }

    mapboxgl.accessToken = mapToken
    const map = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: 'mapbox://styles/mapbox/light-v11',
      center: [-98.5795, 39.8283],
      zoom: 3,
    })

    map.addControl(new mapboxgl.NavigationControl(), 'top-right')

    map.on('load', () => {
      map.addSource('active-fires', {
        type: 'geojson',
        data: EMPTY_GEOJSON,
      })
      map.addSource('selected-cumulative', {
        type: 'geojson',
        data: EMPTY_GEOJSON,
      })
      map.addSource('selected-growth', {
        type: 'geojson',
        data: EMPTY_GEOJSON,
      })
      map.addSource('selected-latest', {
        type: 'geojson',
        data: EMPTY_GEOJSON,
      })

      map.addLayer({
        id: 'selected-cumulative-fill',
        type: 'fill',
        source: 'selected-cumulative',
        paint: {
          'fill-color': ['coalesce', ['get', 'colorHex'], '#f97316'],
          'fill-opacity': [
            'interpolate',
            ['linear'],
            ['coalesce', ['get', 'ageNorm'], 0],
            0,
            0.12,
            1,
            0.45,
          ],
        },
      })

      map.addLayer({
        id: 'selected-growth-fill',
        type: 'fill',
        source: 'selected-growth',
        paint: {
          'fill-color': ['coalesce', ['get', 'colorHex'], '#ea580c'],
          'fill-opacity': 0.55,
        },
      })

      map.addLayer({
        id: 'selected-latest-line',
        type: 'line',
        source: 'selected-latest',
        paint: {
          'line-color': '#7c2d12',
          'line-width': 2.8,
          'line-opacity': 0.95,
        },
      })

      map.addLayer({
        id: 'active-fire-hit',
        type: 'circle',
        source: 'active-fires',
        paint: {
          'circle-radius': 18,
          'circle-opacity': 0,
        },
      })

      map.addLayer({
        id: 'active-fire-circles',
        type: 'circle',
        source: 'active-fires',
        paint: {
          'circle-radius': [
            'interpolate',
            ['linear'],
            ['coalesce', ['get', 'maxAreaKm2'], 0],
            0,
            6,
            5,
            9,
            20,
            13,
            100,
            18,
          ],
          'circle-color': '#fb923c',
          'circle-opacity': 0.82,
          'circle-stroke-color': '#7c2d12',
          'circle-stroke-width': 1.2,
        },
      })

      map.addLayer({
        id: 'active-fire-icons',
        type: 'symbol',
        source: 'active-fires',
        layout: {
          'text-field': '🔥',
          'text-size': [
            'interpolate',
            ['linear'],
            ['coalesce', ['get', 'maxAreaKm2'], 0],
            0,
            15,
            5,
            18,
            20,
            23,
            100,
            30,
          ],
          'text-allow-overlap': true,
        },
      })

      map.on('mouseenter', 'active-fire-hit', () => {
        map.getCanvas().style.cursor = 'pointer'
      })
      map.on('mouseleave', 'active-fire-hit', () => {
        map.getCanvas().style.cursor = ''
      })

      map.on('click', 'active-fire-hit', (event) => {
        const feature = event.features?.[0]
        const properties = feature?.properties ?? {}
        const fireId = Number(properties.fireId)
        if (!Number.isFinite(fireId) || !feature?.geometry?.coordinates) {
          return
        }

        setSelectedFireId(fireId)

        if (popupRef.current) {
          popupRef.current.remove()
        }

        const popupEl = document.createElement('div')
        popupEl.className = 'map-popup'
        const title = document.createElement('div')
        title.className = 'map-popup__title'
        title.textContent = `Fire #${fireId}`
        const body = document.createElement('div')
        body.className = 'map-popup__meta'
        body.innerHTML = `
          <div>Last seen: ${formatUtcDateTime(properties.lastSeen)}</div>
          <div>Max area: ${formatNumber(properties.maxAreaKm2, 2)} km²</div>
          <div>Overpasses: ${formatNumber(properties.overpassCount, 0)}</div>
        `
        popupEl.append(title, body)

        popupRef.current = new mapboxgl.Popup({ offset: 20, closeButton: true })
          .setLngLat(feature.geometry.coordinates)
          .setDOMContent(popupEl)
          .addTo(map)
      })

      setMapReady(true)
    })

    mapRef.current = map

    return () => {
      stopPlayback()
      if (popupRef.current) {
        popupRef.current.remove()
        popupRef.current = null
      }
      map.remove()
      mapRef.current = null
      setMapReady(false)
    }
  }, [mapToken, missingToken, stopPlayback])

  useEffect(() => {
    if (!mapReady) return
    void loadActiveFires()
  }, [mapReady, loadActiveFires])

  useEffect(() => {
    if (!mapReady) return
    void loadSelectedFireDetail()
  }, [mapReady, loadSelectedFireDetail])

  const onRefresh = (event) => {
    event.preventDefault()
    void loadActiveFires()
    if (selectedFireId) {
      void loadSelectedFireDetail()
    }
  }

  const latestSlices = Array.isArray(selectedFireDetail?.series)
    ? [...selectedFireDetail.series].slice(-8).reverse()
    : []

  const latestVisibleSlice = useMemo(() => {
    if (!Array.isArray(selectedFireDetail?.series)) return null
    const visible = selectedFireDetail.series.filter((row) => Number(row.stepIndex) <= visibleStep)
    if (visible.length === 0) return null
    return visible[visible.length - 1]
  }, [selectedFireDetail, visibleStep])

  return (
    <main className="app">
      <h1>Wildfire Viewer</h1>
      <p className="lede">
        Archive-only progressive perimeters: cumulative fire growth is shown from earliest to latest overpass.
      </p>

      <form className="controls" onSubmit={onRefresh}>
        <label>
          As of day (UTC)
          <input
            type="date"
            value={asOfDay}
            onChange={(event) => setAsOfDay(event.target.value)}
          />
        </label>
        <label>
          Lookback days
          <input
            type="number"
            min="1"
            max="30"
            value={lookbackDays}
            onChange={(event) => setLookbackDays(Number(event.target.value) || DEFAULT_LOOKBACK_DAYS)}
          />
        </label>
        <label>
          Min fire size (km²)
          <input
            type="number"
            min="0"
            step="0.1"
            value={minAreaKm2}
            onChange={(event) => setMinAreaKm2(Number(event.target.value) || 0)}
          />
        </label>
        <button type="submit" disabled={missingToken || !mapReady || loadingActive}>
          {loadingActive ? 'Loading...' : 'Refresh'}
        </button>
      </form>

      <p className="status">
        Active fires loaded: <strong>{activeCount}</strong>
      </p>

      {missingToken && (
        <p className="error">
          Set <code>VITE_MAPBOX_TOKEN</code> in a root <code>.env</code> file to display the map.
        </p>
      )}

      {errorMessage && <p className="error">{errorMessage}</p>}

      <section className="layout">
        <div className="map-wrap">
          <div className="legend">
            <span>Earliest</span>
            <div className="legend-swatches">
              {COLOR_STEPS.map((color) => (
                <span key={color} style={{ background: color }} />
              ))}
            </div>
            <span>Latest</span>
          </div>
          <div ref={mapContainerRef} className="map" />
          {loadingDetail && <div className="map-loading">Loading progressive perimeters...</div>}
        </div>

        <aside className="detail-panel">
          <h2>Fire Detail</h2>
          {!selectedFireId && <p className="muted">Click a fire marker to inspect its progression timeline.</p>}

          {selectedFireId && selectedFireDetail && (
            <>
              <p className="detail-id">Fire #{selectedFireId}</p>
              <div className="metrics">
                <div>
                  <span>First seen</span>
                  <strong>{formatUtcDateTime(selectedFireDetail.summary?.firstSeen)}</strong>
                </div>
                <div>
                  <span>Last seen</span>
                  <strong>{formatUtcDateTime(selectedFireDetail.summary?.lastSeen)}</strong>
                </div>
                <div>
                  <span>Max area</span>
                  <strong>{formatNumber(selectedFireDetail.summary?.maxAreaKm2, 2)} km²</strong>
                </div>
                <div>
                  <span>Total FRP</span>
                  <strong>{formatNumber(selectedFireDetail.summary?.totalFrpSum, 1)}</strong>
                </div>
                <div>
                  <span>Overpass steps</span>
                  <strong>{formatNumber(selectedFireDetail.summary?.overpassCount, 0)}</strong>
                </div>
              </div>

              <h3>Timeline</h3>
              <div className="timeline-controls">
                <button
                  type="button"
                  onClick={() => {
                    if (!stepCount) return
                    setVisibleStep(1)
                    setIsPlaying(true)
                  }}
                  disabled={stepCount <= 0}
                >
                  ▶ Play
                </button>
                <button
                  type="button"
                  onClick={stopPlayback}
                  disabled={!isPlaying}
                >
                  ❚❚ Pause
                </button>
                <button
                  type="button"
                  onClick={() => {
                    stopPlayback()
                    setVisibleStep(stepCount)
                  }}
                  disabled={stepCount <= 0}
                >
                  ⤓ Show All
                </button>
              </div>

              <input
                className="timeline-slider"
                type="range"
                min="1"
                max={Math.max(stepCount, 1)}
                value={Math.max(visibleStep, 1)}
                onChange={(event) => {
                  stopPlayback()
                  setVisibleStep(Number(event.target.value))
                }}
                disabled={stepCount <= 0}
              />

              <p className="timeline-status">
                Step <strong>{Math.max(visibleStep, 0)}</strong> / <strong>{stepCount}</strong>
                {latestVisibleSlice && (
                  <span>
                    {' '}· {formatUtcDateTime(latestVisibleSlice.stepTime)} · cumulative area{' '}
                    <strong>{formatNumber(latestVisibleSlice.areaCumulativeKm2, 2)} km²</strong>
                  </span>
                )}
              </p>

              <h3>Cumulative Area</h3>
              <AreaSparkline series={selectedFireDetail.series || []} />

              <h3>Latest Steps</h3>
              {latestSlices.length === 0 && <p className="muted">No steps available.</p>}
              {latestSlices.length > 0 && (
                <ul className="slice-list">
                  {latestSlices.map((slice) => (
                    <li key={slice.stepTime}>
                      <span>{formatUtcDateTime(slice.stepTime)}</span>
                      <strong>{formatNumber(slice.areaCumulativeKm2, 2)} km²</strong>
                    </li>
                  ))}
                </ul>
              )}
            </>
          )}
        </aside>
      </section>

      <p className="request-url">
        Active request: <code>{lastActiveRequest || 'None yet'}</code>
      </p>
      {selectedFireId && (
        <p className="request-url">
          Detail request: <code>{lastDetailRequest || 'None yet'}</code>
        </p>
      )}
    </main>
  )
}

export default App
